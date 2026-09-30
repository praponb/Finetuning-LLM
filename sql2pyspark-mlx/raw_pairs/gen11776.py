from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("sale_date"), "product_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.avg("revenue").over(w).alias("sale_date_month_avg_revenue"),
    )
)
