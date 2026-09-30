from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "store_name",
        F.first("store_name").over(w).alias("first_store_name"),
    )
)
