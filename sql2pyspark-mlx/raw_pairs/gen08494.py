from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "sale_date"), "store_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "revenue",
        F.max("revenue").over(w).alias("sale_date_period_max_revenue"),
    )
)
