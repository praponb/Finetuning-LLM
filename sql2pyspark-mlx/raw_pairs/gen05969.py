from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.sum("revenue").over(w_part).alias("running_sum_revenue"),
        F.count("revenue").over(w_part).alias("running_count_revenue"),
        F.max("revenue").over(w_part).alias("running_max_revenue"),
        F.min("revenue").over(w_part).alias("running_min_revenue"),
    )
)
