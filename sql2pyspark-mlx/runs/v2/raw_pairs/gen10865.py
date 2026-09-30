from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("sqft").desc(), "opened_date")
)
