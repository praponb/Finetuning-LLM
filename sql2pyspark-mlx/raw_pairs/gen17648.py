from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "TH", "US"]))
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.round(F.avg("rating").over(w), 2).alias("rating_moving_avg_7"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)
