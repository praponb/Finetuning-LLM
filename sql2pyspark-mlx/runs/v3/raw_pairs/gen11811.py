from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "region",
        "sqft",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy(F.col("opened_date").desc())
)
