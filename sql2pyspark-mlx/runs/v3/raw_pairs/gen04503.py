from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("age", "id")

result = (
    df_customers
    .filter(F.lower(F.trim("name")) == "david lee")
    .select("id", "country", "segment", "age", F.row_number().over(w).alias("rn"))
)
