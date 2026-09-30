from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "store_id",
        "units",
        F.sum("revenue").over(w).alias("running_revenue"),
        (F.col("units") - F.lag("units", 1).over(w2)).alias("units_change"),
    )
    .orderBy(F.col("units").desc(), F.col("id").desc())
)
