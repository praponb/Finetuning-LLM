from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        "units",
        "sale_date",
        F.rank().over(w).alias("rnk"),
        F.ntile(10).over(w2).alias("bucket"),
    )
    .orderBy(F.col("units").desc())
)
