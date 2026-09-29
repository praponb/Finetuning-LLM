from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("region").isin(["South", "Central"]))
    .select(
        "id",
        "region",
        "opened_date",
        "city",
        "sqft",
        F.rank().over(w).alias("rnk"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
