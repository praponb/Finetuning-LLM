from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc())
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "store_id",
        F.rank().over(w).alias("rnk"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
    .orderBy(F.col("units_share").desc())
)
