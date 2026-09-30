from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("opened_date", "id")

scored = (
    df_stores
    .withColumn("region_sum_sqft", F.sum("sqft").over(w))
    .withColumn("last_known_region", F.last("region", ignorenulls=True).over(w2))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w3))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("prev_sqft", F.lag("sqft", 2).over(w4))
)

result = (
    scored
    .groupBy("region", "city")
    .agg(
        F.round(F.avg("sqft_share"), 2).alias("avg_sqft_share"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("avg_sqft_share") < 0)
    .orderBy(F.col("region").asc_nulls_first())
)
