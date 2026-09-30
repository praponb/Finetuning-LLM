from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "units",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 3).alias("revenue_share"),
        F.ntile(2).over(w2).alias("units_ntile"),
    )
    .orderBy(F.col("revenue").desc())
)
