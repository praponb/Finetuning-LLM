from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .select("id", "rating", F.sum("rating").over(w).alias("running_rating"))
    .orderBy("running_rating")
)
