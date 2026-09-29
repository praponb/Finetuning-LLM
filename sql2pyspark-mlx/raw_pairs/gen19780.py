from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.count("revenue").over(w).alias("running_count_revenue"),
        F.sum("revenue").over(w).alias("running_sum_revenue"),
        F.avg("revenue").over(w).alias("running_avg_revenue"),
    )
)
