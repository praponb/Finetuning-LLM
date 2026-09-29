from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "region",
        "sqft",
        F.row_number().over(w).alias("row_num"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
