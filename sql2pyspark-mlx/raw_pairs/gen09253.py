from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft_whole").desc())

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_anti")
)

cleaned = (
    not_flagged
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.floor("sqft").alias("sqft_whole"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("rn_2", F.rank().over(w2))
    .filter(F.col("rn_2") == 1)
)

result = (
    latest
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "sqft_whole",
        "resolved_region",
        "rn_2",
    )
)
