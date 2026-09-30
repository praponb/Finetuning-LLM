from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 5, 7]))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.round((F.col("units") / F.sum("units").over(w)), 3).alias("units_share"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("product_id").desc(), F.col("revenue").desc())
)
