from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "store_id",
        "revenue",
        F.rank().over(w).alias("rnk"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 3).alias("revenue_share"),
    )
    .orderBy(F.col("store_id").desc())
)
