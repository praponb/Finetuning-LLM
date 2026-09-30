from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w2 = Window.orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)

scored = (
    df_stores
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("sqft_avg_5", F.avg("sqft").over(w2))
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "sqft_avg_5",
        F.when(F.col("region").rlike("^(North|East)$"), "OTHER_ZONE").when(F.col("region").rlike("^(South|West)$"), "GROUP_B").otherwise("PRIORITY").alias("region_zone"),
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(5)
)
