from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_products
    .filter(F.year("launch_date") == 2024)
    .select(
        "id",
        "cost",
        "stock_qty",
        "supplier_id",
        F.count("cost").over(w_recent).alias("last7_count_cost"),
        F.min("cost").over(w_recent).alias("last7_min_cost"),
        F.sum("cost").over(w_recent).alias("last7_sum_cost"),
    )
)

result = (
    scored
    .select("id", "stock_qty", "last7_sum_cost")
    .orderBy(F.col("stock_qty").asc_nulls_last(), F.col("last7_sum_cost").desc())
)
