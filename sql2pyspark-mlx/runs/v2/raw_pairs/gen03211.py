from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "sale_date",
        "revenue",
        F.row_number().over(w).alias("rn"),
        F.lag("revenue", 2).over(w2).alias("prev_revenue"),
    )
)
