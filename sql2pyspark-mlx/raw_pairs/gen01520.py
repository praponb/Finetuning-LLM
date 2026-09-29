from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.orderBy("units")

result = (
    df_store_sales
    .select(
        "id",
        "revenue",
        "units",
        F.last("units").over(w).alias("last_units"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
    )
    .orderBy(F.col("units_dense_rank").desc(), "units")
)
