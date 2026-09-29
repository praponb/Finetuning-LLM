from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.lead("rating", 2, 0).over(w).alias("next_rating"),
    )
    .orderBy("rating", F.col("country").desc())
)
