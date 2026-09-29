from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units").between(32, 34))
    .select(
        "id",
        "product_id",
        "sale_date",
        "store_id",
        "revenue",
        F.row_number().over(w).alias("sale_date_row"),
        F.lead("revenue", 1).over(w2).alias("next_revenue"),
    )
    .orderBy(F.col("next_revenue").desc())
)
