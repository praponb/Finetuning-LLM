from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.orderBy(F.col("sqft").desc(), "id")
w5 = Window.partitionBy("region")

enriched = (
    df_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_3_prior"),
        F.avg("sqft").over(w2).alias("sqft_avg_5"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
        F.ntile(10).over(w4).alias("sqft_bucket"),
        F.count("sqft").over(w5).alias("region_count_sqft"),
    )
)

result = (
    enriched
    .select(
        "id",
        "city",
        "opened_date",
        "sqft_avg_3_prior",
        "sqft_avg_5",
        "sqft_share",
        "region_count_sqft",
    )
)
