from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.ntile(10).over(w).alias("units_bucket"),
        F.lag("units", 1).over(w2).alias("prev_units"),
    )
)
