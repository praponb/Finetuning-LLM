from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

metrics = (
    df_stores
    .filter(F.col("sqft") == 13169)
    .select(
        "*",
        F.avg("sqft").over(w).alias("last4_avg_sqft"),
        F.max("sqft").over(w).alias("last4_max_sqft"),
        F.sum("sqft").over(w).alias("last4_sum_sqft"),
        F.min("sqft").over(w).alias("last4_min_sqft"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "sqft",
        "last4_max_sqft",
        "last4_sum_sqft",
        "last4_min_sqft",
    )
)
