from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "Tokyo", "Singapore"]))
    .select(
        "id",
        "country",
        "rating",
        F.avg("rating").over(w).alias("country_avg_rating"),
    )
)
