from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.row_number().over(w2).alias("revenue_row"),
    )
)
