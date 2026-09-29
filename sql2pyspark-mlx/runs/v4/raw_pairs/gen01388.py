from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("rating_tier").orderBy(F.col("fashion_rows").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select("*", F.lower("country").alias("country_lower"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.brand").alias("product_brand"), F.col("c.category"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date").between("2023-09-01", "2023-11-09")), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([4, 22, 33])), "left_anti")
)

result = (
    not_flagged_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW").alias("rating_tier"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.min("rating").alias("min_rating"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
    )
    .withColumn("fashion_rows_percent_rank", F.percent_rank().over(w2))
)
