from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.year(F.col("c.sale_date")) == 2023), "left_semi")
)

kept = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_anti")
)

latest = (
    kept
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    latest
    .filter((F.col("opened_date") >= "2025-04-01") & (F.col("opened_date") > F.to_date(F.lit("2023-05-24"))))
    .withColumn("is_north", F.when(F.col("region") == "North", 1).otherwise(0))
)

eligible_stores = (
    base
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_anti")
)

wide = (
    eligible_stores
    .groupBy("city")
    .pivot("region", ["West", "South"])
    .agg(F.max("sqft"))
)

result = wide.select("city", "West")
