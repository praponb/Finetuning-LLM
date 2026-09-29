from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("country") != "TH")
    .select("id", "country", "rating", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "country", "rating")
    .orderBy("country")
)
