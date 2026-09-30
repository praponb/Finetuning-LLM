from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id", "product_id")
w_running = Window.partitionBy(F.date_trunc("MONTH", "sale_date"), "store_id")

result = (
    df_store_sales
    .filter(F.col("units").isNull())
    .select(
        "id",
        "product_id",
        "revenue",
        F.row_number().over(w).alias("row_seq"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
        F.sum("units").over(w_running).alias("sale_date_period_sum_units"),
    )
)
