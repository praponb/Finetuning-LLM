from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("region") == "North")
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
    )
)
