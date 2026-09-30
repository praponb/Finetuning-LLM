from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("sqft").between(10398, 12608))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.percent_rank().over(w2).alias("opened_date_pct_rank"),
    )
)
