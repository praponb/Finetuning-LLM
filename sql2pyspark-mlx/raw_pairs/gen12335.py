from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_store_sales
    .select(
        "*",
        F.sum("units").over(w).alias("last4_sum_units"),
        F.avg("units").over(w).alias("last4_avg_units"),
        F.count("units").over(w).alias("last4_count_units"),
        F.max("units").over(w).alias("last4_max_units"),
    )
)
