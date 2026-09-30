from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("rating") < 3.1)
    .select(
        "id",
        "country",
        "rating",
        F.min("rating").over(w).alias("country_min_rating"),
    )
)
