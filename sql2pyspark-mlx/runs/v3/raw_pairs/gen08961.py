from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") > 3.1)
    .select("id", "city", "rating", F.sum("rating").over(w).alias("running_rating"))
    .orderBy("city", F.col("running_rating").desc())
)
