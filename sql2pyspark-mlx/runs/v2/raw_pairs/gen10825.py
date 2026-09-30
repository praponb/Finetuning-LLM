from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 3).alias("revenue_share"),
    )
)
