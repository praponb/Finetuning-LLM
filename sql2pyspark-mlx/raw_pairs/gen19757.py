from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-04-18")
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w_running).alias("running_sum_sqft"),
        F.avg("sqft").over(w_running).alias("running_avg_sqft"),
        F.max("sqft").over(w_running).alias("running_max_sqft"),
    )
)
