from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("country") != "JP")
    .select("id", "rating", F.lead("rating", 1).over(w).alias("next_rating"))
)
