from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("store_sale_sale_date")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.length("store_name") < 6))
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.when(F.col("city").isin(["Bangkok", "Chiang Mai"]), "Group A").otherwise("Other").alias("city_group"),
        F.concat_ws("-", "region", "store_name").alias("region_store_name_key"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.units").between(30, 36)), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.product_id").isin([1, 2, 9, 10])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"), F.col("c.units"))
)

latest = (
    latest_match
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

eligible_stores = (
    latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.store_sale_sale_date")), "left_anti")
)

result = (
    eligible_stores
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "city_group",
        "region_store_name_key",
        "store_sale_sale_date",
        "row_num",
    )
    .orderBy("city")
)
