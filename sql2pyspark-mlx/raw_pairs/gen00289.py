from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_last())

latest = (
    df_stores
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select(
        "id",
        "store_name",
        "sqft",
        "latest_rank",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "GROUP_B").otherwise("EXTENDED").alias("store_name_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "sqft")
)
