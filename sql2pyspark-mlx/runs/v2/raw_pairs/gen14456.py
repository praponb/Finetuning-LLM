from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(~F.col("city").isin(["New York", "London", "Singapore"]))
    .select("id", "rating", F.row_number().over(w).alias("rn"))
)
