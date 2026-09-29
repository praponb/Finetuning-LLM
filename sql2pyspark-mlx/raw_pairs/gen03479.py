from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.avg("revenue").over(w).alias("running_avg_revenue"),
        F.min("revenue").over(w).alias("running_min_revenue"),
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.max("revenue").over(w).alias("running_max_revenue"),
        F.sum("revenue").over(w).alias("cumulative_revenue"),
    )
)
