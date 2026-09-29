from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, -1)

enriched = (
    df_stores
    .withColumn("running_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_avg_3_prior", F.avg("sqft").over(w2))
    .withColumn("sqft_avg_6_prior", F.avg("sqft").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_avg_3_prior",
        "sqft_avg_6_prior",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
