from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("sqft").between(6079, 8645))
    .select(
        "id",
        "store_name",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(4).over(w3).alias("sqft_bucket"),
    )
)
