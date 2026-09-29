from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w4 = Window.orderBy("sale_date", "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(-3, -1)
discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.sum("units").over(w2).alias("cumulative_units"),
        F.lead("units", 1).over(w3).alias("next_units"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("revenue").over(w5).alias("revenue_avg_3_prior"),
    )
)

result = (
    with_windows
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "corr_revenue_units",
        "next_units",
        "revenue_avg_3_prior",
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("corr_revenue_units").desc())
)
