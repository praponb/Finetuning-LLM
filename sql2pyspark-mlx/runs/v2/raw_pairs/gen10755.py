from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "units",
        F.lag("revenue", 1, 0).over(w).alias("prev_revenue"),
        F.rank().over(w2).alias("rnk"),
    )
)
