from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.row_number().over(w).alias("rn"),
        F.row_number().over(w2).alias("rn_2"),
    )
)
