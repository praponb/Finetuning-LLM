from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("sqft") == 7017)
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "city",
        F.lead("sqft", 1, 0).over(w).alias("next_sqft"),
        F.rank().over(w2).alias("opened_date_rank"),
    )
)
