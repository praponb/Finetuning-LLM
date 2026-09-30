from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "sale_date"), "product_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 3).alias("revenue_share"),
    )
)
