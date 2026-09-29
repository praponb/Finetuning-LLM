from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.count("revenue").over(w_part).alias("running_count_revenue"),
        F.sum("revenue").over(w_part).alias("running_sum_revenue"),
        F.max("revenue").over(w_part).alias("running_max_revenue"),
    )
)
