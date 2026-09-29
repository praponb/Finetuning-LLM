from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-05-01")
    .select(
        "id",
        "sale_date",
        "revenue",
        (F.col("revenue") - F.lag("revenue", 2).over(w)).alias("revenue_change"),
    )
    .orderBy(F.col("revenue").desc())
)
