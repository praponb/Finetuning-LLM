from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("supplier_id", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")

with_windows = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "stock_qty",
        F.avg("cost").over(w_part).alias("running_avg_cost"),
        F.sum("cost").over(w_part).alias("running_sum_cost"),
        F.min("cost").over(w_part).alias("running_min_cost"),
        F.row_number().over(w_grp).alias("row_seq"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "price",
        "stock_qty",
        "running_sum_cost",
        "running_min_cost",
        "row_seq",
    )
)
