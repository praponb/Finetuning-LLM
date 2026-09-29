from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc())

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= "2024-02-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.floor("units").alias("units_whole"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.sqft") >= 7564)
    .select(
        "e.id",
        "e.store_id",
        "e.sale_date",
        "e.units",
        "e.units_whole",
        "e.latest_rank",
        F.col("b.sqft"),
        F.col("b.region"),
        F.col("b.store_name"),
    )
)

cleaned = (
    enriched
    .filter(F.col("region").isNotNull() & (F.col("region") == "Central"))
    .select(
        "id",
        "sale_date",
        "units",
        "units_whole",
        "sqft",
        "region",
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
    )
)

result = (
    cleaned
    .select("id", "units", "sqft", "sale_date_dayofweek")
    .orderBy(F.col("sale_date_dayofweek").desc())
)
