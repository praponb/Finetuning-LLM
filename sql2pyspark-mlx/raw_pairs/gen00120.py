from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("city_length").desc(), "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.price") <= 830), "left_semi")
)

cleaned = (
    kept
    .filter(F.col("rating") <= 2.7)
    .select(
        "*",
        F.when(F.col("country").isin(["SG", "JP"]), "Core").otherwise("Other").alias("country_group"),
        F.when(F.col("country").isin(["DE", "TH"]), "Priority").otherwise("Other").alias("country_group_2"),
    )
)

valid = (
    cleaned
    .filter(F.col("city") == "New York")
    .withColumn("city_length", F.length("city"))
    .withColumn("supplier_name_country_key", F.concat_ws("/", "supplier_name", "country"))
)

eligible = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.brand") == "Umbrella"), "left_semi")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty") != 152), "left_anti")
)

latest = (
    eligible_suppliers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        (F.col("rn") / F.nullif("city_length", F.lit(0))).alias("rn_per_city_length"),
    )
)

result = (
    base
    .groupBy("city", "country")
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.percentile_approx("city_length", 0.95).alias("city_length_p95"),
    )
    .filter(F.col("city_length_p95") < 1000000)
    .orderBy("city")
)
