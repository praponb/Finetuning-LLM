from pyspark.sql import functions as F

result = df_suppliers.agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
