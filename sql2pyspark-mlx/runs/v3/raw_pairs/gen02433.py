from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        "sale_date",
        "revenue",
        F.max("units").over(w).alias("store_id_max_units"),
        (F.col("revenue") - F.lag("revenue", 1).over(w2)).alias("revenue_change"),
    )
)
