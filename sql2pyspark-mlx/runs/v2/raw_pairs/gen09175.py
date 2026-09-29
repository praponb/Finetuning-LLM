from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-01", "2025-06-01"))
    .select(
        "id",
        "product_id",
        "units",
        "sale_date",
        "revenue",
        F.row_number().over(w).alias("rn"),
        (F.col("revenue") - F.lag("revenue", 2).over(w2)).alias("revenue_change"),
    )
)
