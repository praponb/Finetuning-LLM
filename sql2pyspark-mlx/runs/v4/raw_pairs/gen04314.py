from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "latest_rank",
        F.when(F.col("region").rlike("^(West|Central)$"), "GROUP_A").when(F.col("region").rlike("^(North|South)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("region_zone"),
    )
    .orderBy(F.col("opened_date").desc(), F.col("city").desc())
)
