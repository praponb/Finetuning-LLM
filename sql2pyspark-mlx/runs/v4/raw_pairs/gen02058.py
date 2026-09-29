from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.min("rating").over(w).alias("country_min_rating"),
    )
)
