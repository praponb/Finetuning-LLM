from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("product_cost").desc_nulls_first(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.lower("supplier_name").alias("supplier_name_lower"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("product_cost"))
)

ranked = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") != "Home"), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.countDistinct("country").alias("distinct_countries"),
        F.sum("rating").alias("total_rating"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.round(F.sum("product_cost") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("product_cost_per_us"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.max("product_cost").alias("max_product_cost"),
    )
    .orderBy(F.col("max_product_cost").desc_nulls_first())
)
