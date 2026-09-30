from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country")

metrics = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.stddev_samp("rating").over(w2).alias("country_stddev_samp_rating"),
    )
)

result = (
    metrics
    .select("id", "city", "next_rating")
    .orderBy("city")
)
