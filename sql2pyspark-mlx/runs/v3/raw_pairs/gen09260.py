from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") < 3.3)
    .select("id", "country", "rating", F.lead("rating", 1).over(w).alias("next_rating"))
)
