from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "London", "New York"]))
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.first("city").over(w).alias("first_city"),
    )
)
